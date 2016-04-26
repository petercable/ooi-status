"""Added stream condition

Revision ID: f3d59112a529
Revises: 6e549fab2959
Create Date: 2016-04-25 16:13:05.477878

"""

# revision identifiers, used by Alembic.
revision = 'f3d59112a529'
down_revision = '6e549fab2959'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('stream_condition',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('stream_id', sa.Integer(), nullable=False),
        sa.Column('last_status_time', sa.DateTime(), nullable=False),
        sa.Column('last_status', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(['stream_id'], ['deployed_stream.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('stream_id')
    )


def downgrade():
    op.drop_table('stream_condition')
